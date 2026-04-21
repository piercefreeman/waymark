'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { registerHooks } = require('node:module');
const { pathToFileURL, fileURLToPath } = require('node:url');

const { ACTION_BOOTSTRAP_FILENAME } = require('../compiler/bootstrap.js');

const BOOTSTRAP_ENV_VAR = 'WAYMARK_JS_BOOTSTRAP';
const BOOTSTRAP_RELATIVE_PATH = path.join('.waymark', ACTION_BOOTSTRAP_FILENAME);
const RESOLVABLE_EXTENSIONS = ['.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs'];

const bootstrapLoadCache = new Map();
const registeredProjectRoots = new Set();

function resolveBootstrapPath(options = {}) {
  const env = options.env || process.env;
  const cwd = path.resolve(options.cwd || process.cwd());
  const explicitPath = options.bootstrapPath || env[BOOTSTRAP_ENV_VAR];

  if (explicitPath) {
    const resolvedExplicitPath = path.resolve(cwd, explicitPath);
    assertFileExists(
      resolvedExplicitPath,
      `Waymark bootstrap not found at ${resolvedExplicitPath}`
    );
    return resolvedExplicitPath;
  }

  const discoveredPath = findBootstrapPath(cwd);
  if (discoveredPath) {
    return discoveredPath;
  }

  throw new Error(
    `Could not find ${BOOTSTRAP_RELATIVE_PATH} from ${cwd}. ` +
      `Set ${BOOTSTRAP_ENV_VAR} or pass --bootstrap to the worker runtime.`
  );
}

function findBootstrapPath(startDirectory) {
  let currentDirectory = path.resolve(startDirectory);

  while (true) {
    const candidate = path.join(currentDirectory, BOOTSTRAP_RELATIVE_PATH);
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return candidate;
    }

    const parentDirectory = path.dirname(currentDirectory);
    if (parentDirectory === currentDirectory) {
      return null;
    }

    currentDirectory = parentDirectory;
  }
}

function projectRootForBootstrap(bootstrapPath) {
  return path.dirname(path.dirname(path.resolve(bootstrapPath)));
}

async function loadBootstrap(options = {}) {
  const bootstrapPath = resolveBootstrapPath(options);
  const cacheKey = bootstrapPath;

  if (!bootstrapLoadCache.has(cacheKey)) {
    const projectRoot = projectRootForBootstrap(bootstrapPath);
    registerProjectModuleHooks(projectRoot);
    bootstrapLoadCache.set(cacheKey, import(pathToFileURL(bootstrapPath).href));
  }

  await bootstrapLoadCache.get(cacheKey);
  return bootstrapPath;
}

function registerProjectModuleHooks(projectRoot) {
  const normalizedProjectRoot = path.resolve(projectRoot);
  if (registeredProjectRoots.has(normalizedProjectRoot)) {
    return;
  }

  const pathAliases = loadProjectPathAliases(normalizedProjectRoot);

  registerHooks({
    resolve(specifier, context, nextResolve) {
      const aliasedPath = resolveAliasedProjectPath(specifier, normalizedProjectRoot, pathAliases);
      if (aliasedPath) {
        return resolveProjectModule(aliasedPath);
      }

      const projectRelativePath = resolveProjectRelativePath(specifier, context.parentURL, normalizedProjectRoot);
      if (projectRelativePath) {
        return resolveProjectModule(projectRelativePath);
      }

      return nextResolve(specifier, context);
    }
  });

  registeredProjectRoots.add(normalizedProjectRoot);
}

function loadProjectPathAliases(projectRoot) {
  const tsconfigPath = path.join(projectRoot, 'tsconfig.json');
  if (!fs.existsSync(tsconfigPath)) {
    return [];
  }

  let tsconfig = null;
  try {
    tsconfig = JSON.parse(fs.readFileSync(tsconfigPath, 'utf8'));
  } catch {
    return [];
  }

  const compilerOptions = tsconfig.compilerOptions || {};
  const paths = compilerOptions.paths || {};
  const baseUrl = path.resolve(projectRoot, compilerOptions.baseUrl || '.');

  return Object.entries(paths).map(([pattern, targets]) => ({
    pattern,
    targets: Array.isArray(targets)
      ? targets.map((target) => path.resolve(baseUrl, target))
      : []
  }));
}

function resolveAliasedProjectPath(specifier, projectRoot, pathAliases) {
  for (const alias of pathAliases) {
    const wildcard = matchPattern(specifier, alias.pattern);
    if (wildcard === null) {
      continue;
    }

    for (const targetPattern of alias.targets) {
      const candidateBase = applyWildcard(targetPattern, wildcard);
      const resolvedCandidate = resolveCandidatePath(candidateBase);
      if (resolvedCandidate) {
        return resolvedCandidate;
      }
    }
  }

  if (specifier.startsWith('@/')) {
    const candidateBase = path.join(projectRoot, specifier.slice(2));
    const resolvedCandidate = resolveCandidatePath(candidateBase);
    if (resolvedCandidate) {
      return resolvedCandidate;
    }
  }

  return null;
}

function resolveProjectRelativePath(specifier, parentURL, projectRoot) {
  if (!parentURL || (!specifier.startsWith('./') && !specifier.startsWith('../') && !path.isAbsolute(specifier))) {
    return null;
  }

  const parentPath = fileURLToPath(parentURL);
  const candidateBase = path.resolve(path.dirname(parentPath), specifier);
  if (!isPathInsideProject(candidateBase, projectRoot)) {
    return null;
  }

  return resolveCandidatePath(candidateBase);
}

function resolveCandidatePath(candidateBase) {
  const candidates = [candidateBase];

  if (!path.extname(candidateBase)) {
    for (const extension of RESOLVABLE_EXTENSIONS) {
      candidates.push(`${candidateBase}${extension}`);
    }
    for (const extension of RESOLVABLE_EXTENSIONS) {
      candidates.push(path.join(candidateBase, `index${extension}`));
    }
  }

  for (const candidate of candidates) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return candidate;
    }
  }

  return null;
}

function resolveProjectModule(resolvedPath) {
  const resolution = {
    shortCircuit: true,
    url: pathToFileURL(resolvedPath).href
  };

  const format = moduleFormatForResolvedPath(resolvedPath);
  if (format) {
    resolution.format = format;
  }

  return resolution;
}

function moduleFormatForResolvedPath(resolvedPath) {
  switch (path.extname(resolvedPath)) {
    case '.ts':
    case '.tsx':
      return 'module-typescript';
    default:
      return null;
  }
}

function matchPattern(specifier, pattern) {
  if (!pattern.includes('*')) {
    return specifier === pattern ? '' : null;
  }

  const [prefix, suffix] = pattern.split('*');
  if (!specifier.startsWith(prefix) || !specifier.endsWith(suffix)) {
    return null;
  }

  return specifier.slice(prefix.length, specifier.length - suffix.length);
}

function applyWildcard(targetPattern, wildcard) {
  if (!targetPattern.includes('*')) {
    return targetPattern;
  }
  return targetPattern.replace('*', wildcard);
}

function isPathInsideProject(candidatePath, projectRoot) {
  const relativePath = path.relative(projectRoot, candidatePath);
  return relativePath === '' || (!relativePath.startsWith('..') && !path.isAbsolute(relativePath));
}

function assertFileExists(candidatePath, message) {
  if (!fs.existsSync(candidatePath) || !fs.statSync(candidatePath).isFile()) {
    throw new Error(message);
  }
}

module.exports = {
  BOOTSTRAP_ENV_VAR,
  BOOTSTRAP_RELATIVE_PATH,
  findBootstrapPath,
  loadBootstrap,
  moduleFormatForResolvedPath,
  projectRootForBootstrap,
  registerProjectModuleHooks,
  resolveBootstrapPath
};
