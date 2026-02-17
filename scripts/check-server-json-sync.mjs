import { readFileSync } from "node:fs";
import { resolve } from "node:path";

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

const packagePath = resolve(process.cwd(), "package.json");
const serverPath = resolve(process.cwd(), "server.json");

const pkg = readJson(packagePath);
const server = readJson(serverPath);

const errors = [];

if (typeof pkg.version !== "string" || pkg.version.length === 0) {
  errors.push("package.json.version is missing or invalid");
}

if (typeof server.version !== "string" || server.version.length === 0) {
  errors.push("server.json.version is missing or invalid");
}

if (pkg.version !== server.version) {
  errors.push(
    `Version mismatch: package.json.version=${pkg.version} but server.json.version=${server.version}`,
  );
}

if (typeof pkg.mcpName === "string" && pkg.mcpName.length > 0) {
  if (server.name !== pkg.mcpName) {
    errors.push(
      `Name mismatch: package.json.mcpName=${pkg.mcpName} but server.json.name=${server.name}`,
    );
  }
}

const packageEntries = Array.isArray(server.packages) ? server.packages : [];
const npmEntry = packageEntries.find(
  (entry) =>
    entry &&
    typeof entry === "object" &&
    entry.registryType === "npm" &&
    entry.identifier === pkg.name,
);

if (!npmEntry) {
  errors.push(`server.json.packages is missing npm entry with identifier=${pkg.name}`);
} else if (npmEntry.version !== pkg.version) {
  errors.push(
    `NPM package version mismatch: server.json.packages[npm:${pkg.name}].version=${npmEntry.version} but package.json.version=${pkg.version}`,
  );
}

if (errors.length > 0) {
  console.error("server.json/package.json consistency check failed:");
  for (const error of errors) {
    console.error(`- ${error}`);
  }
  process.exit(1);
}

console.log(
  `server.json and package.json are consistent (version ${pkg.version}, name ${server.name}).`,
);
