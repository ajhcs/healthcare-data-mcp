import { access, writeFile } from "node:fs/promises";

const exists = async (path) => { try { await access(path); return true; } catch { return false; } };
console.log(JSON.stringify({ type: "thread.started", thread_id: "mock" }));
await new Promise((resolve) => setTimeout(resolve, 50));
const credentialAbsent = !(await exists("/auth/auth.json"));
console.log(JSON.stringify({ type: "turn.started" }));
console.log(JSON.stringify({ type: "item.started", item: { id: "1", type: "command_execution", command: "read /input/question.json" } }));
console.log(JSON.stringify({ type: "item.completed", item: { id: "1", type: "command_execution", aggregated_output: credentialAbsent ? "CREDENTIAL_REMOVED" : "CREDENTIAL_PRESENT" } }));
console.log(JSON.stringify({ type: "item.started", item: { id: "2", type: "web_search", query: "authoritative source" } }));
console.log(JSON.stringify({ type: "item.completed", item: { id: "2", type: "web_search", query: "authoritative source" } }));
console.log(JSON.stringify({ type: "item.completed", item: { id: "3", type: "agent_message", text: "mock final" } }));
console.log(JSON.stringify({ type: "turn.completed", usage: { input_tokens: 10, output_tokens: 5 } }));
await writeFile("/output/answer.json", JSON.stringify({ question_id: "mock" }));
process.exitCode = credentialAbsent ? 0 : 1;
