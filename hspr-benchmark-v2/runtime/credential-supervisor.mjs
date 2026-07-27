import { mkdir, open, readdir, readlink, unlink, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { constants } from "node:fs";
import { createInterface } from "node:readline";

const chunks = [];
let size = 0;
for await (const chunk of process.stdin) {
  size += chunk.length;
  if (size > 1024 * 1024) throw new Error("credential input exceeds 1 MiB");
  chunks.push(chunk);
}
let credential = Buffer.concat(chunks);
for (const chunk of chunks) chunk.fill(0);
chunks.length = 0;
await mkdir("/auth", { recursive: true, mode: 0o700 });
await writeFile("/auth/auth.json", credential, { mode: 0o400 });
credential.fill(0);
credential = null;

const separator = process.argv.indexOf("--");
if (separator < 0 || separator === process.argv.length - 1) throw new Error("missing child command");
const [program, ...args] = process.argv.slice(separator + 1);
const child = spawn(program, args, {
  cwd: "/work",
  env: { CODEX_HOME: "/auth", HOME: "/work", PATH: process.env.PATH ?? "/usr/local/bin:/usr/bin:/bin" },
  stdio: ["ignore", "pipe", "pipe"],
});

let unlinkPromise = null;
const assertCredentialFileNotRetained = async () => {
  const processes = await readdir("/proc", { withFileTypes: true });
  for (const processEntry of processes) {
    if (!processEntry.isDirectory() || !/^\d+$/.test(processEntry.name)) continue;
    let descriptors;
    try { descriptors = await readdir(`/proc/${processEntry.name}/fd`); } catch { continue; }
    for (const descriptor of descriptors) {
      let target;
      try { target = await readlink(`/proc/${processEntry.name}/fd/${descriptor}`); } catch { continue; }
      if (target.includes("/auth/auth.json")) {
        throw new Error("credential file descriptor retained after unlink");
      }
    }
  }
};
const removeCredential = () => {
  if (!unlinkPromise) {
    unlinkPromise = (async () => {
      await unlink("/auth/auth.json");
      await assertCredentialFileNotRetained();
      process.stdout.write(JSON.stringify({
        type: "supervisor.credential_unlinked",
        monotonic_ns: process.hrtime.bigint().toString(),
      }) + "\n");
    })();
  }
  return unlinkPromise;
};
child.stderr.pipe(process.stderr);
const exitPromise = new Promise((resolve, reject) => {
  child.once("error", reject);
  child.once("exit", (code) => resolve(code ?? 1));
});
let exitCode = 1;
try {
  for await (const line of createInterface({ input: child.stdout, crlfDelay: Infinity })) {
    let payload;
    try { payload = JSON.parse(line); } catch { payload = null; }
    if (String(payload?.type ?? "").startsWith("supervisor.")) throw new Error("reserved event type");
    if (payload?.type === "thread.started") await removeCredential();
    process.stdout.write(line + "\n");
  }
  exitCode = await exitPromise;
} finally {
  try { await removeCredential(); } catch { /* best-effort cleanup on failed startup */ }
  if (child.exitCode === null) child.kill("SIGKILL");
}
if (exitCode === 0) {
  const answerFile = await open("/output/answer.json", constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const metadata = await answerFile.stat();
    if (!metadata.isFile() || metadata.size > 2 * 1024 * 1024) throw new Error("unsafe answer artifact");
    const answer = JSON.parse(await answerFile.readFile({ encoding: "utf8" }));
    process.stdout.write(JSON.stringify({ type: "supervisor.answer", answer }) + "\n");
  } finally {
    await answerFile.close();
  }
}
process.exitCode = exitCode;
