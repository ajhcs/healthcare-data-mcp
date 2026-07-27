import { access, open, readdir, readFile, readlink } from "node:fs/promises";
import { constants } from "node:fs";
import { lookup } from "node:dns/promises";

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
const credentialEnvironmentVisible = async () => {
  for (const entry of await readdir("/proc", { withFileTypes: true })) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;
    try {
      const environment = await readFile(`/proc/${entry.name}/environ`, { encoding: "utf8" });
      if (/(refresh_token|access_token|id_token|OPENAI_API_KEY)/.test(environment)) return true;
    } catch { /* inaccessible or exited process */ }
  }
  return false;
};
const processMemoryReadable = async (pid) => {
  let handle;
  try { handle = await open(`/proc/${pid}/mem`, "r"); return true; } catch { return false; }
  finally { await handle?.close(); }
};
const anyOtherProcessMemoryReadable = async () => {
  for (const entry of await readdir("/proc", { withFileTypes: true })) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name) || Number(entry.name) === process.pid) continue;
    if (await processMemoryReadable(entry.name)) return true;
  }
  return false;
};
const anyOtherProcessIpcReopenable = async () => {
  for (const entry of await readdir("/proc", { withFileTypes: true })) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name) || Number(entry.name) === process.pid) continue;
    let descriptors;
    try { descriptors = await readdir(`/proc/${entry.name}/fd`); } catch { continue; }
    for (const descriptor of descriptors) {
      let target;
      try { target = await readlink(`/proc/${entry.name}/fd/${descriptor}`); } catch { continue; }
      if (!target.startsWith("pipe:[") && !target.startsWith("socket:[")) continue;
      let handle;
      try {
        handle = await open(`/proc/${entry.name}/fd/${descriptor}`, constants.O_RDONLY | constants.O_NONBLOCK);
        return true;
      } catch { /* ptrace policy or descriptor type denied access */ }
      finally { await handle?.close(); }
    }
  }
  return false;
};
const shellNetworkDenied = async () => {
  try { await lookup("example.com"); return false; } catch { return true; }
};

const checks = {
  auth_path_absent: !(await exists("/auth/auth.json")),
  no_retained_auth_fd: !(await credentialFdRetained()),
  no_credential_environment: !(await credentialEnvironmentVisible()),
  all_other_process_memory_denied: !(await anyOtherProcessMemoryReadable()),
  all_other_process_ipc_fds_denied: !(await anyOtherProcessIpcReopenable()),
  shell_network_denied: await shellNetworkDenied(),
};
const passed = Object.values(checks).every(Boolean);
console.log(JSON.stringify({
  event: "live_boundary_probe",
  passed,
  checks,
  marker: passed ? "REAL_CONTEXT_CREDENTIAL_INACCESSIBLE" : "REAL_CONTEXT_BOUNDARY_FAILED",
}));
process.exitCode = passed ? 0 : 1;
