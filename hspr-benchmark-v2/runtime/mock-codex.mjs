import { access, open, readFile, readdir, readlink, writeFile } from "node:fs/promises";

const exists = async (path) => { try { await access(path); return true; } catch { return false; } };
const credentialFdRetained = async () => {
  for (const entry of await readdir("/proc", { withFileTypes: true })) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;
    let descriptors;
    try { descriptors = await readdir(`/proc/${entry.name}/fd`); } catch { continue; }
    for (const descriptor of descriptors) {
      try {
        if ((await readlink(`/proc/${entry.name}/fd/${descriptor}`)).includes("/auth/auth.json")) return true;
      } catch { /* process or descriptor exited */ }
    }
  }
  return false;
};
const parentMemoryReadable = async () => {
  let handle;
  try { handle = await open(`/proc/${process.ppid}/mem`, "r"); return true; } catch { return false; }
  finally { await handle?.close(); }
};
console.log(JSON.stringify({ type: "thread.started", thread_id: "mock" }));
await new Promise((resolve) => setTimeout(resolve, 50));
const credentialAbsent = !(await exists("/auth/auth.json"));
const noCredentialFd = !(await credentialFdRetained());
const parentEnvironment = await readFile(`/proc/${process.ppid}/environ`, { encoding: "utf8" });
const noCredentialEnvironment = !/(refresh_token|access_token|id_token|OPENAI_API_KEY)/.test(parentEnvironment);
const parentMemoryDenied = !(await parentMemoryReadable());
const credentialInaccessible = credentialAbsent && noCredentialFd && noCredentialEnvironment && parentMemoryDenied;
console.log(JSON.stringify({ type: "turn.started" }));
console.log(JSON.stringify({ type: "item.started", item: { id: "1", type: "command_execution", command: "read /input/question.json" } }));
console.log(JSON.stringify({ type: "item.completed", item: { id: "1", type: "command_execution", aggregated_output: credentialInaccessible ? "CREDENTIAL_INACCESSIBLE" : "CREDENTIAL_EXPOSED" } }));
console.log(JSON.stringify({ type: "item.started", item: { id: "2", type: "web_search", query: "authoritative source" } }));
console.log(JSON.stringify({ type: "item.completed", item: { id: "2", type: "web_search", query: "authoritative source" } }));
console.log(JSON.stringify({ type: "item.completed", item: { id: "3", type: "agent_message", text: "mock final" } }));
console.log(JSON.stringify({ type: "turn.completed", usage: { input_tokens: 10, output_tokens: 5 } }));
await writeFile("/output/answer.json", JSON.stringify({ question_id: "mock" }));
process.exitCode = credentialInaccessible ? 0 : 1;
