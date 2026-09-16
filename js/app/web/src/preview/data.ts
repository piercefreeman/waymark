import type { Status } from "../components/status-badge";
import type { TimelineItem } from "../components/timeline";

export interface PreviewAction extends TimelineItem {
  input?: unknown;
  response?: unknown;
}

export interface PreviewWorkflow {
  id: string;
  name: string;
  module: string;
  status: Status;
  duration: string;
  node: string;
  actions: PreviewAction[];
}

// Explicit design fixtures. Never substitute these for an API response.
export const workflows: PreviewWorkflow[] = [
  {
    id: "019a7e21-6ad0-7000-8000-a1b2c3d4e5f6",
    name: "OrderFulfillment",
    module: "commerce.orders",
    status: "running",
    duration: "1.42 s",
    node: "worker-01",
    actions: [
      {
        id: "fetch",
        name: "fetch_order",
        status: "success",
        start: 0,
        duration: 180,
        input: { order_id: "ord_10482" },
        response: {
          id: "ord_10482",
          customer: "cus_218",
          total: 149.0,
          currency: "USD",
          items: [{ sku: "WM-042", quantity: 2 }],
        },
      },
      {
        id: "validate",
        name: "validate_inventory",
        status: "success",
        start: 200,
        duration: 320,
        input: { sku: "WM-042", quantity: 2 },
        response: { available: true, reserved: 2 },
      },
      {
        id: "charge",
        name: "charge_payment",
        status: "running",
        start: 550,
        duration: 870,
        input: { order_id: "ord_10482", amount: 149, currency: "USD" },
      },
      {
        id: "notify",
        name: "send_confirmation",
        status: "waiting",
        start: 0,
        duration: null,
      },
    ],
  },
  {
    id: "019a7e21-7ee1-7000-8000-b1c2d3e4f5a6",
    name: "DocumentIngestion",
    module: "documents.pipeline",
    status: "running",
    duration: "8.21 s",
    node: "worker-02",
    actions: [
      {
        id: "download",
        name: "download_document",
        status: "success",
        start: 0,
        duration: 800,
        input: { document_id: "doc_042" },
        response: { bytes: 238120 },
      },
      {
        id: "extract",
        name: "extract_text",
        status: "running",
        start: 840,
        duration: 7370,
        input: { document_id: "doc_042", language: "en" },
      },
    ],
  },
  {
    id: "019a7e20-1234-7000-8000-c1d2e3f4a5b6",
    name: "CustomerEnrichment",
    module: "customers.enrichment",
    status: "success",
    duration: "842 ms",
    node: "worker-01",
    actions: [
      {
        id: "lookup",
        name: "lookup_company",
        status: "success",
        start: 0,
        duration: 510,
        input: { domain: "example.com" },
        response: { name: "Example", employees: 240 },
      },
      {
        id: "save",
        name: "save_profile",
        status: "success",
        start: 530,
        duration: 312,
        input: { customer_id: "cus_218" },
        response: { updated: true },
      },
    ],
  },
  {
    id: "019a7e20-5678-7000-8000-d1e2f3a4b5c6",
    name: "InvoiceReconciliation",
    module: "billing.invoices",
    status: "failed",
    duration: "2.18 s",
    node: "worker-03",
    actions: [
      {
        id: "load",
        name: "load_invoice",
        status: "success",
        start: 0,
        duration: 360,
        input: { invoice_id: "inv_702" },
        response: { amount: 149, currency: "USD" },
      },
      {
        id: "reconcile",
        name: "reconcile_payment",
        status: "failed",
        start: 400,
        duration: 1780,
        input: { invoice_id: "inv_702" },
        response: {
          error: "PaymentMismatch",
          message: "Received 129.00 USD; expected 149.00 USD.",
        },
      },
    ],
  },
  {
    id: "019a7e1f-aaaa-7000-8000-e1f2a3b4c5d6",
    name: "CatalogSync",
    module: "catalog.sync",
    status: "running",
    duration: "24.6 s",
    node: "worker-02",
    actions: [
      {
        id: "catalog",
        name: "fetch_catalog",
        status: "success",
        start: 0,
        duration: 3100,
        response: { products: 2400 },
      },
      {
        id: "index",
        name: "rebuild_index",
        status: "running",
        start: 3200,
        duration: 21400,
        input: { batch_size: 100 },
      },
    ],
  },
  {
    id: "019a7e1e-bbbb-7000-8000-f1a2b3c4d5e6",
    name: "DailyDigest",
    module: "notifications.digest",
    status: "waiting",
    duration: "1m 12s",
    node: "worker-01",
    actions: [
      {
        id: "collect",
        name: "collect_updates",
        status: "success",
        start: 0,
        duration: 600,
        response: { updates: 18 },
      },
      {
        id: "schedule",
        name: "wait_until_delivery",
        status: "waiting",
        start: 620,
        duration: null,
      },
    ],
  },
  {
    id: "019a7e1e-cccc-7000-8000-a2b3c4d5e6f7",
    name: "WebhookDelivery",
    module: "integrations.webhooks",
    status: "failing",
    duration: "4.80 s",
    node: "worker-03",
    actions: [
      {
        id: "sign",
        name: "sign_payload",
        status: "success",
        start: 0,
        duration: 80,
        response: { signed: true },
      },
      {
        id: "deliver",
        name: "deliver_webhook",
        status: "failed",
        start: 100,
        duration: 4700,
        input: { event: "order.completed", attempt: 3 },
        response: {
          error: "ConnectionTimeout",
          message: "The receiving endpoint did not respond.",
        },
      },
    ],
  },
  {
    id: "019a7e1d-dddd-7000-8000-b2c3d4e5f6a7",
    name: "ExportDataset",
    module: "analytics.exports",
    status: "success",
    duration: "12.3 s",
    node: "worker-02",
    actions: [
      {
        id: "query",
        name: "query_records",
        status: "success",
        start: 0,
        duration: 8200,
        input: { collection: "orders", range: "last_7_days" },
        response: { records: 42180 },
      },
      {
        id: "upload",
        name: "upload_export",
        status: "success",
        start: 8250,
        duration: 4050,
        response: { file: "orders.csv", bytes: 4810024 },
      },
    ],
  },
];

export const workers = [
  {
    name: "worker-01",
    processes: 8,
    used: 48,
    capacity: 80,
    queued: 4,
    rate: "32.4",
    latency: "184 ms",
    samples: [14, 24, 18, 36, 28, 34, 29, 45, 42, 48, 40, 48],
  },
  {
    name: "worker-02",
    processes: 8,
    used: 62,
    capacity: 80,
    queued: 12,
    rate: "41.8",
    latency: "216 ms",
    samples: [20, 22, 32, 31, 44, 48, 39, 51, 54, 60, 52, 62],
  },
  {
    name: "worker-03",
    processes: 4,
    used: 16,
    capacity: 40,
    queued: 0,
    rate: "12.6",
    latency: "142 ms",
    samples: [8, 10, 14, 11, 10, 16, 13, 18, 17, 13, 15, 16],
  },
];
