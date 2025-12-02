let installed = false;
let originalPrototype;
let originalWindow;
let hadWindow = false;

function FakeWorkerGlobalScope() {}
FakeWorkerGlobalScope.prototype.set_timeout_with_callback_and_timeout_and_arguments_0 =
  function fakeSetTimeout(cb, ms) {
    return setTimeout(cb, ms);
  };

export function install_fake_worker_scope() {
  if (installed) {
    return true;
  }

  try {
    originalPrototype = Object.getPrototypeOf(globalThis);
    hadWindow = Object.prototype.hasOwnProperty.call(globalThis, "window");
    originalWindow = globalThis.window;

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
    installed = true;
    return true;
  } catch (_err) {
    return false;
  }
}

export function restore_fake_worker_scope() {
  if (!installed) {
    return false;
  }

  try {
    Object.setPrototypeOf(globalThis, originalPrototype);
    if (hadWindow) {
      globalThis.window = originalWindow;
    } else {
      delete globalThis.window;
    }
    delete globalThis.WorkerGlobalScope;
  } catch (_err) {
    // Ignore restoration errors to avoid masking test results.
  } finally {
    installed = false;
  }

  return true;
}
