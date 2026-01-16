import { runAction } from "@rappel/js/workflow";

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// =============================================================================
// Shared Types
// =============================================================================

export type ComputationResult = {
  input_number: number;
  factorial: number;
  fibonacci: number;
  summary: string;
};

export type ChainResult = {
  original: string;
  steps: string[];
  final: string;
};

export type BranchResult = {
  value: number;
  branch_taken: "high" | "medium" | "low";
  message: string;
};

export type LoopResult = {
  items: string[];
  processed: string[];
  count: number;
};

export type LoopReturnResult = {
  items: number[];
  needle: number;
  found: boolean;
  value: number | null;
  checked: number;
};

export type ErrorResult = {
  attempted: boolean;
  recovered: boolean;
  message: string;
  error_type?: string | null;
  error_code?: number | null;
  error_detail?: string | null;
};

export type SleepResult = {
  started_at: string;
  resumed_at: string;
  sleep_seconds: number;
  message: string;
};

export type GuardFallbackResult = {
  user: string;
  note_count: number;
  summary: string;
};

export type KwOnlyLocationResult = {
  latitude: number | null;
  longitude: number | null;
  message: string;
};

export type ParseResult = {
  session_id: string | null;
  items: string[];
  new_items: string[];
};

export type ProcessedItemResult = {
  item_id: string;
  processed: boolean;
};

export type EarlyReturnLoopResult = {
  had_session: boolean;
  processed_count: number;
  all_items: string[];
};

export type LoopExceptionResult = {
  items: string[];
  processed: string[];
  error_count: number;
  message: string;
};

export type SpreadEmptyResult = {
  items_processed: number;
  message: string;
};

export type NoOpResult = {
  count: number;
  even_count: number;
  odd_count: number;
};

export type NoOpTag = {
  value: number;
  tag: string;
};

// =============================================================================
// Errors
// =============================================================================

export class IntentionalError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IntentionalError";
  }
}

export class ExceptionMetadataError extends Error {
  code: number;
  detail: string;

  constructor(message: string, code: number, detail: string) {
    super(message);
    this.name = "ExceptionMetadataError";
    this.code = code;
    this.detail = detail;
  }
}

export class ItemProcessingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ItemProcessingError";
  }
}

// =============================================================================
// Actions - Parallel Workflow
// =============================================================================

export async function computeFactorial(n: number): Promise<number> {
  "use action";

  let total = 1;
  for (let value = 2; value <= n; value += 1) {
    total *= value;
    await delay(0);
  }
  return total;
}

export async function computeFibonacci(n: number): Promise<number> {
  "use action";

  let previous = 0;
  let current = 1;
  for (let idx = 0; idx < n; idx += 1) {
    const next = previous + current;
    previous = current;
    current = next;
    await delay(0);
  }
  return previous;
}

export async function summarizeMath(
  input_number: number,
  factorial_value: number,
  fibonacci_value: number
): Promise<ComputationResult> {
  "use action";

  let summary = "";
  if (factorial_value > 5000) {
    summary = `${input_number}! is massive compared to Fib(${input_number})=${fibonacci_value}`;
  } else if (factorial_value > 100) {
    summary = `${input_number}! is larger, but Fibonacci is ${fibonacci_value}`;
  } else {
    summary = `${input_number}! (${factorial_value}) stays tame next to Fibonacci=${fibonacci_value}`;
  }
  return {
    input_number,
    factorial: factorial_value,
    fibonacci: fibonacci_value,
    summary,
  };
}

// =============================================================================
// Actions - Sequential Chain
// =============================================================================

export async function stepUppercase(text: string): Promise<string> {
  "use action";

  await delay(0);
  return text.toUpperCase();
}

export async function stepReverse(text: string): Promise<string> {
  "use action";

  await delay(0);
  return text.split("").reverse().join("");
}

export async function stepAddStars(text: string): Promise<string> {
  "use action";

  await delay(0);
  return `*** ${text} ***`;
}

export async function buildChainResult(
  original: string,
  step1: string,
  step2: string,
  step3: string
): Promise<ChainResult> {
  "use action";

  return {
    original,
    steps: [step1, step2, step3],
    final: step3,
  };
}

// =============================================================================
// Actions - Conditional Branch
// =============================================================================

export async function evaluateHigh(value: number): Promise<BranchResult> {
  "use action";

  return {
    value,
    branch_taken: "high",
    message: `Value ${value} is considered high`,
  };
}

export async function evaluateMedium(value: number): Promise<BranchResult> {
  "use action";

  return {
    value,
    branch_taken: "medium",
    message: `Value ${value} is considered medium`,
  };
}

export async function evaluateLow(value: number): Promise<BranchResult> {
  "use action";

  return {
    value,
    branch_taken: "low",
    message: `Value ${value} is considered low`,
  };
}

// =============================================================================
// Actions - Loop Processing
// =============================================================================

export async function processItem(item: string): Promise<string> {
  "use action";

  await delay(0);
  return `processed:${item}`;
}

export async function processItemsList(items: string[]): Promise<string[]> {
  "use action";

  const processed: string[] = [];
  for (const item of items) {
    await delay(0);
    processed.push(`processed:${item}`);
  }
  return processed;
}

export async function buildLoopResult(
  items: string[],
  processed: string[]
): Promise<LoopResult> {
  "use action";

  return {
    items,
    processed,
    count: processed.length,
  };
}

// =============================================================================
// Actions - Return Inside Loop
// =============================================================================

export async function matchesNeedle(
  value: number,
  needle: number
): Promise<boolean> {
  "use action";

  await delay(0);
  return value === needle;
}

export async function buildLoopReturnResult(
  items: number[],
  needle: number,
  found: boolean,
  value: number | null,
  checked: number
): Promise<LoopReturnResult> {
  "use action";

  return {
    items,
    needle,
    found,
    value,
    checked,
  };
}

// =============================================================================
// Actions - Error Handling
// =============================================================================

export async function riskyAction(should_fail: boolean): Promise<string> {
  "use action";

  await delay(100);
  if (should_fail) {
    throw new IntentionalError("This action failed as requested!");
  }
  return "Action completed successfully";
}

export async function recoveryAction(error_message: string): Promise<string> {
  "use action";

  await delay(100);
  return `Recovered from error: ${error_message}`;
}

export async function successAction(result: string): Promise<string> {
  "use action";

  await delay(100);
  return `Success path: ${result}`;
}

export async function buildErrorResult(
  attempted: boolean,
  recovered: boolean,
  message: string,
  error_type: string | null = null,
  error_code: number | null = null,
  error_detail: string | null = null
): Promise<ErrorResult> {
  "use action";

  return {
    attempted,
    recovered,
    message,
    error_type,
    error_code,
    error_detail,
  };
}

export async function riskyMetadataAction(
  should_fail: boolean
): Promise<string> {
  "use action";

  await delay(100);
  if (should_fail) {
    throw new ExceptionMetadataError("Metadata error triggered", 418, "teapot");
  }
  return "Metadata action completed";
}

// =============================================================================
// Actions - Sleep Workflow
// =============================================================================

export async function getTimestamp(): Promise<string> {
  "use action";

  return new Date().toISOString();
}

export async function durableSleep(seconds: number): Promise<number> {
  "use action";

  await delay(seconds * 1000);
  return seconds;
}

export async function formatSleepResult(
  started: string,
  resumed: string,
  seconds: number
): Promise<SleepResult> {
  "use action";

  return {
    started_at: started,
    resumed_at: resumed,
    sleep_seconds: seconds,
    message: `Slept for ${seconds} seconds between ${started} and ${resumed}`,
  };
}

// =============================================================================
// Actions - Early Return with Loop
// =============================================================================

export async function parseInputData(input_text: string): Promise<ParseResult> {
  "use action";

  await delay(50);
  if (input_text.startsWith("no_session:")) {
    return {
      session_id: null,
      items: [],
      new_items: [],
    };
  }

  const items = input_text
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return {
    session_id: "session-123",
    items,
    new_items: items,
  };
}

export async function processSingleItem(
  item: string,
  session_id: string
): Promise<ProcessedItemResult> {
  "use action";

  await delay(50);
  return {
    item_id: `processed-${item}`,
    processed: true,
  };
}

export async function finalizeProcessing(
  items: string[],
  processed_count: number
): Promise<EarlyReturnLoopResult> {
  "use action";

  await delay(50);
  return {
    had_session: true,
    processed_count,
    all_items: items,
  };
}

export async function buildEmptyResult(): Promise<EarlyReturnLoopResult> {
  "use action";

  return {
    had_session: false,
    processed_count: 0,
    all_items: [],
  };
}

// =============================================================================
// Actions - Guard Fallback Workflow
// =============================================================================

export async function fetchRecentNotes(user: string): Promise<string[]> {
  "use action";

  await delay(50);
  if (user.toLowerCase() === "empty") {
    return [];
  }
  return [`${user}-note-1`, `${user}-note-2`];
}

export async function summarizeNotes(notes: string[]): Promise<string> {
  "use action";

  await delay(50);
  return notes.join(" | ");
}

export async function buildGuardFallbackResult(
  user: string,
  note_count: number,
  summary: string
): Promise<GuardFallbackResult> {
  "use action";

  await delay(0);
  return {
    user,
    note_count,
    summary,
  };
}

export async function describeLocation(
  latitude: number | null = null,
  longitude: number | null = null
): Promise<KwOnlyLocationResult> {
  "use action";

  if (latitude === null || longitude === null) {
    return {
      latitude,
      longitude,
      message: "Location inputs are optional; provide both for a precise pin.",
    };
  }
  return {
    latitude,
    longitude,
    message: `Resolved location at ${latitude.toFixed(4)}, ${longitude.toFixed(4)}.`,
  };
}

export async function echoExternal(value: string): Promise<string> {
  "use action";

  return value;
}

// =============================================================================
// Actions - Loop with Exception Handling
// =============================================================================

export async function processItemMayFail(item: string): Promise<string> {
  "use action";

  await delay(50);
  if (item.toLowerCase().startsWith("bad")) {
    throw new ItemProcessingError(`Failed to process item: ${item}`);
  }
  return `processed:${item}`;
}

export async function processItemsWithFailures(
  items: string[]
): Promise<LoopExceptionResult> {
  "use action";

  const processed: string[] = [];
  let error_count = 0;

  for (const item of items) {
    try {
      const result = await processItemMayFail(item);
      processed.push(result);
    } catch (error) {
      error_count += 1;
    }
  }

  let message = "";
  if (error_count === 0) {
    message = `All ${processed.length} items processed successfully`;
  } else if (error_count === items.length) {
    message = `All ${error_count} items failed processing`;
  } else {
    message = `Processed ${processed.length} items, ${error_count} failures`;
  }

  return {
    items,
    processed,
    error_count,
    message,
  };
}

export async function buildLoopExceptionResult(
  items: string[],
  processed: string[],
  error_count: number
): Promise<LoopExceptionResult> {
  "use action";

  let message = "";
  if (error_count === 0) {
    message = `All ${processed.length} items processed successfully`;
  } else if (error_count === items.length) {
    message = `All ${error_count} items failed processing`;
  } else {
    message = `Processed ${processed.length} items, ${error_count} failures`;
  }
  return {
    items,
    processed,
    error_count,
    message,
  };
}

// =============================================================================
// Actions - Spread Empty Collection
// =============================================================================

export async function processSpreadItem(item: string): Promise<string> {
  "use action";

  await delay(50);
  return `processed:${item}`;
}

export async function processSpreadItems(items: string[]): Promise<string[]> {
  "use action";

  const results: string[] = [];
  for (const item of items) {
    results.push(await processSpreadItem(item));
  }
  return results;
}

export async function buildSpreadEmptyResult(
  results: string[]
): Promise<SpreadEmptyResult> {
  "use action";

  const count = results.length;
  const message =
    count === 0
      ? "No items to process - empty spread handled correctly!"
      : `Processed ${count} items: ${results.join(", ")}`;
  return {
    items_processed: count,
    message,
  };
}

// =============================================================================
// Actions - No-op Workflow
// =============================================================================

export async function noopInt(value: number): Promise<number> {
  "use action";

  return value;
}

export async function noopTagFromValue(value: number): Promise<NoOpTag> {
  "use action";

  const tag = value % 2 === 0 ? "even" : "odd";
  return { value, tag };
}

export async function noopCombine(items: NoOpTag[]): Promise<NoOpResult> {
  "use action";

  const even_count = items.filter((item) => item.tag === "even").length;
  const odd_count = items.length - even_count;
  return {
    count: items.length,
    even_count,
    odd_count,
  };
}

export async function runNoOpWorkflow(
  indices: number[],
  _complexity: number
): Promise<NoOpResult> {
  "use action";

  const tagged = indices.map((value) => ({
    value,
    tag: value % 2 === 0 ? "even" : "odd",
  }));
  const even_count = tagged.filter((item) => item.tag === "even").length;
  return {
    count: tagged.length,
    even_count,
    odd_count: tagged.length - even_count,
  };
}

// =============================================================================
// Workflow Definitions
// =============================================================================

export async function ParallelMathWorkflow(
  number: number
): Promise<ComputationResult> {
  "use workflow";

  const factorial_value = await computeFactorial(number);
  const fibonacci_value = await computeFibonacci(number);
  const result = await summarizeMath(number, factorial_value, fibonacci_value);
  return result;
}

export async function SequentialChainWorkflow(
  text: string
): Promise<ChainResult> {
  "use workflow";

  const step1 = await stepUppercase(text);
  const step2 = await stepReverse(step1);
  const step3 = await stepAddStars(step2);
  const result = await buildChainResult(text, step1, step2, step3);
  return result;
}

export async function ConditionalBranchWorkflow(
  value: number
): Promise<BranchResult> {
  "use workflow";

  if (value >= 75) {
    const result = await evaluateHigh(value);
    return result;
  }
  if (value >= 25) {
    const result = await evaluateMedium(value);
    return result;
  }
  const result = await evaluateLow(value);
  return result;
}

export async function LoopProcessingWorkflow(
  items: string[]
): Promise<LoopResult> {
  "use workflow";

  const processed = await processItemsList(items);
  const result = await buildLoopResult(items, processed);
  return result;
}

export async function LoopReturnWorkflow(
  items: number[],
  needle: number
): Promise<LoopReturnResult> {
  "use workflow";

  let checked = 0;
  for (const value of items) {
    checked = checked + 1;
    const is_match = await matchesNeedle(value, needle);
    if (is_match) {
      const result = await buildLoopReturnResult(
        items,
        needle,
        true,
        value,
        checked
      );
      return result;
    }
  }
  const result = await buildLoopReturnResult(items, needle, false, null, checked);
  return result;
}

export async function ErrorHandlingWorkflow(
  should_fail: boolean
): Promise<ErrorResult> {
  "use workflow";

  let recovered = false;
  let message = "";

  try {
    const result = await runAction(riskyAction(should_fail), {
      retry: { attempts: 1 },
    });
    message = await successAction(result);
  } catch (error) {
    recovered = true;
    message = await recoveryAction("IntentionalError was caught");
  }

  const result = await buildErrorResult(true, recovered, message);
  return result;
}

export async function ExceptionMetadataWorkflow(
  should_fail: boolean
): Promise<ErrorResult> {
  "use workflow";

  let recovered = false;
  let message = "";
  let error_type: string | null = null;
  let error_code: number | null = null;
  let error_detail: string | null = null;

  try {
    const result = await runAction(riskyMetadataAction(should_fail), {
      retry: { attempts: 1 },
    });
    message = await successAction(result);
  } catch (error: { code?: number; detail?: string }) {
    recovered = true;
    error_type = "ExceptionMetadataError";
    if (error.code) {
      error_code = error.code;
    }
    if (error.detail) {
      error_detail = error.detail;
    }
    message = await recoveryAction("Captured exception metadata");
  }

  const result = await buildErrorResult(
    true,
    recovered,
    message,
    error_type,
    error_code,
    error_detail
  );
  return result;
}

export async function DurableSleepWorkflow(
  seconds: number
): Promise<SleepResult> {
  "use workflow";

  const started = await getTimestamp();
  await durableSleep(seconds);
  const resumed = await getTimestamp();
  const result = await formatSleepResult(started, resumed, seconds);
  return result;
}

export async function EarlyReturnLoopWorkflow(
  input_text: string
): Promise<EarlyReturnLoopResult> {
  "use workflow";

  const parse_result = await parseInputData(input_text);
  if (!parse_result.session_id) {
    const result = await buildEmptyResult();
    return result;
  }

  let processed_count = 0;
  for (const item of parse_result.new_items) {
    await processSingleItem(item, parse_result.session_id);
    processed_count = processed_count + 1;
  }

  const result = await finalizeProcessing(parse_result.items, processed_count);
  return result;
}

export async function GuardFallbackWorkflow(
  user: string
): Promise<GuardFallbackResult> {
  "use workflow";

  const notes = await fetchRecentNotes(user);
  let summary = "no notes found";
  if (notes.length) {
    summary = await summarizeNotes(notes);
  }
  const result = await buildGuardFallbackResult(user, notes.length, summary);
  return result;
}

export async function KwOnlyLocationWorkflow(
  latitude: number | null = null,
  longitude: number | null = null
): Promise<KwOnlyLocationResult> {
  "use workflow";

  const result = await describeLocation(latitude, longitude);
  return result;
}

const GLOBAL_FALLBACK = "external-default";

export async function UndefinedVariableWorkflow(
  input_text: string
): Promise<string> {
  "use workflow";

  const result = await echoExternal(GLOBAL_FALLBACK);
  return result;
}

export async function LoopExceptionWorkflow(
  items: string[]
): Promise<LoopExceptionResult> {
  "use workflow";

  const result = await processItemsWithFailures(items);
  return result;
}

export async function SpreadEmptyCollectionWorkflow(
  items: string[]
): Promise<SpreadEmptyResult> {
  "use workflow";

  const results = await processSpreadItems(items);
  const result = await buildSpreadEmptyResult(results);
  return result;
}

export async function NoOpWorkflow(
  indices: number[],
  complexity: number = 0
): Promise<NoOpResult> {
  "use workflow";

  const result = await runNoOpWorkflow(indices, complexity);
  return result;
}

export const ExampleMathWorkflow = ParallelMathWorkflow;
