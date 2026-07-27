import { readFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

const chunks = [];
let size = 0;
for await (const chunk of process.stdin) {
  size += chunk.length;
  if (size > 1024 * 1024) throw new Error("credential input exceeds 1 MiB");
  chunks.push(chunk);
}
let credentialBytes = Buffer.concat(chunks);
for (const chunk of chunks) chunk.fill(0);
chunks.length = 0;
const credential = JSON.parse(credentialBytes.toString("utf8"));
credentialBytes.fill(0);
credentialBytes = null;
if (
  credential?.auth_mode !== "chatgpt" ||
  credential?.OPENAI_API_KEY != null ||
  typeof credential?.tokens?.access_token !== "string" ||
  typeof credential?.tokens?.account_id !== "string" ||
  Object.keys(credential.tokens).sort().join(",") !== "access_token,account_id"
) {
  throw new Error("invalid minimized subscription credential");
}

const separator = process.argv.indexOf("--");
if (separator < 0 || process.argv.length < separator + 4) throw new Error("missing runtime arguments");
const [model, reasoning, prompt] = process.argv.slice(separator + 1);
const allowedModelReasoning = new Set([
  "gpt-5.6-luna:medium",
  "gpt-5.6-luna:xhigh",
  "gpt-5.6-sol:medium",
]);
if (!allowedModelReasoning.has(`${model}:${reasoning}`)) {
  throw new Error("unlocked model or reasoning configuration");
}
const outputSchema = JSON.parse(await readFile("/input/response-schema.json", "utf8"));
const child = spawn(
  "node",
  ["/opt/codex/bin/codex.js", "--search", "app-server", "--stdio"],
  {
    cwd: "/work",
    env: { CODEX_HOME: "/auth", HOME: "/work", PATH: process.env.PATH ?? "/usr/local/bin:/usr/bin:/bin" },
    stdio: ["pipe", "pipe", "pipe"],
  },
);
child.stderr.pipe(process.stderr);

let nextId = 1;
const pending = new Map();
let turnActive = false;
let finalText = null;
let completedTurn = null;
const normalizeItem = (item) => {
  const type = {
    agentMessage: "agent_message",
    commandExecution: "command_execution",
    webSearch: "web_search",
  }[item?.type] ?? item?.type;
  return {
    ...item,
    type,
    aggregated_output: item?.aggregatedOutput,
  };
};
const emit = (payload) => process.stdout.write(JSON.stringify(payload) + "\n");
const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
lines.on("line", (line) => {
  let message;
  try { message = JSON.parse(line); } catch { return; }
  if (message.id != null && pending.has(message.id)) {
    const { resolve, reject } = pending.get(message.id);
    pending.delete(message.id);
    if (message.error) reject(new Error(`app-server request failed: ${message.error.code ?? "unknown"}`));
    else resolve(message.result);
    return;
  }
  if (!turnActive || typeof message.method !== "string") return;
  if (message.method === "item/started" || message.method === "item/completed") {
    const phase = message.method.endsWith("started") ? "started" : "completed";
    const item = normalizeItem(message.params?.item ?? {});
    if (phase === "completed" && item.type === "agent_message") finalText = item.text;
    emit({ type: `item.${phase}`, item });
  } else if (message.method === "turn/completed") {
    completedTurn = message.params?.turn ?? {};
  }
});
const request = (method, params) => {
  const id = nextId++;
  const result = new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
  child.stdin.write(JSON.stringify({ id, method, params }) + "\n");
  return result;
};
const notify = (method, params) => child.stdin.write(JSON.stringify({ method, params }) + "\n");
const waitForTurn = async () => {
  const deadline = Date.now() + 600_000;
  while (completedTurn == null) {
    if (Date.now() >= deadline) throw new Error("app-server turn timed out");
    if (child.exitCode != null) throw new Error("app-server exited before turn completion");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

try {
  await request("initialize", {
    clientInfo: { name: "hspr-isolated-answer", version: "1" },
    capabilities: { experimentalApi: true },
  });
  notify("initialized", {});
  const accessToken = credential.tokens.access_token;
  const accountId = credential.tokens.account_id;
  await request("account/login/start", {
    type: "chatgptAuthTokens",
    accessToken,
    chatgptAccountId: accountId,
  });
  credential.tokens.access_token = "";
  credential.tokens.account_id = "";
  emit({ type: "supervisor.external_auth_ready", monotonic_ns: process.hrtime.bigint().toString() });
  const started = await request("thread/start", {
    model,
    cwd: "/work",
    approvalPolicy: "never",
    sandbox: "read-only",
    ephemeral: true,
    experimentalRawEvents: true,
    config: {
      model_reasoning_effort: reasoning,
      shell_environment_policy: { inherit: "none" },
      features: { use_legacy_landlock: true },
    },
  });
  const threadId = started?.thread?.id;
  if (typeof threadId !== "string") throw new Error("thread/start returned no thread id");
  emit({ type: "thread.started", thread_id: threadId });
  turnActive = true;
  const turn = await request("turn/start", {
    threadId,
    input: [{ type: "text", text: prompt }],
    model,
    effort: reasoning,
    outputSchema,
    approvalPolicy: "never",
    cwd: "/work",
  });
  emit({ type: "turn.started", turn_id: turn?.turn?.id ?? null });
  await waitForTurn();
  emit({ type: "turn.completed", usage: completedTurn?.usage ?? null });
  if (finalText == null) {
    finalText = [...(completedTurn?.items ?? [])].reverse().find((item) => item?.type === "agentMessage")?.text ?? null;
  }
  if (completedTurn?.status !== "completed") {
    throw new Error(`app-server turn failed: ${JSON.stringify(completedTurn?.error ?? { status: completedTurn?.status })}`);
  }
  if (typeof finalText !== "string") throw new Error("turn completed without an agent message");
  const answer = JSON.parse(finalText);
  emit({ type: "supervisor.answer", answer });
} finally {
  child.stdin.end();
  if (child.exitCode == null) child.kill("SIGTERM");
}
