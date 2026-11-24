import { readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";

const root = resolve("dist");
const alias = /(["'])#src\/([^"']+)\.js(["'])/g;

const leftovers = [];

async function walk(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) await walk(path);
    else if (path.endsWith(".js") || path.endsWith(".d.ts")) {
      const out = await rewrite(path);
      // A new alias shape that the regex misses must fail the build, not ship a
      // `#src/` specifier Node cannot resolve.
      if (out.includes("#src/")) leftovers.push(path);
    }
  }
}

async function rewrite(file) {
  const source = await readFile(file, "utf8");
  const out = source.replace(alias, (_match, open, target, close) => {
    const absolute = join(root, target + ".js");
    let spec = relative(dirname(file), absolute).replaceAll("\\", "/");
    if (!spec.startsWith(".")) spec = `./${spec}`;
    return `${open}${spec}${close}`;
  });
  if (out !== source) await writeFile(file, out);
  return out;
}

await walk(root);
if (leftovers.length > 0) {
  console.error(`unrewritten #src aliases: ${leftovers.join(", ")}`);
  process.exit(1);
}
