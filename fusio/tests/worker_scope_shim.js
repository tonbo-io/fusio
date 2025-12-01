export function install_fake_worker_scope() {
  try {
    class FakeWorkerGlobalScope {
      set_timeout_with_callback_and_timeout_and_arguments_0(cb, ms) {
        return setTimeout(cb, ms);
      }
    }
    // Provide WorkerGlobalScope constructor so `instanceof` checks pass.
    Object.defineProperty(globalThis, "WorkerGlobalScope", {
      value: FakeWorkerGlobalScope,
      configurable: true,
      writable: true,
    });

    try {
      // Hide window so the executor will pick the worker path if possible.
      delete globalThis.window;
    } catch (_err) {
      // Ignore if the platform disallows this; fallback to skip.
    }

    // Make the global object look like a WorkerGlobalScope instance.
    Object.setPrototypeOf(globalThis, FakeWorkerGlobalScope.prototype);
    return true;
  } catch (_err) {
    return false;
  }
}
