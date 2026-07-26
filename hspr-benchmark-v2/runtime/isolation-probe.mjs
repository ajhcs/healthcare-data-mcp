import { access, constants, writeFile } from "node:fs/promises";

const stamp = () => process.hrtime.bigint().toString();
const exists = async (path) => {
  try { await access(path, constants.F_OK); return true; } catch { return false; }
};
const checks = {
  packet_visible: await exists("/input/question.json"),
  repo_hidden: !(await exists(process.env.PROBE_REPO_PATH)),
  sealed_hidden: !(await exists(process.env.PROBE_SEALED_PATH)),
  docker_socket_hidden: !(await exists("/var/run/docker.sock")),
  host_root_alias_hidden: !(await exists("/host")),
};
try { await writeFile("/output/probe-write.json", JSON.stringify({ok:true})); checks.output_writable = true; }
catch { checks.output_writable = false; }
const passed = Object.values(checks).every(Boolean);
console.log(JSON.stringify({event:"probe_result", monotonic_ns:stamp(), passed, checks}));
process.exitCode = passed ? 0 : 1;
