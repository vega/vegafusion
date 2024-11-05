// By storing them in a variable, we can keep `Worker` objects around and
// prevent them from getting GC-d.
let _workers;

export async function makeRuntimeQueryFn() {
  const worker = new Worker(
    new URL('./workerHelpers.worker.js', import.meta.url),
    {
      type: 'module'
    }
  );

  _workers = [worker];

  return (data) => {
    worker.postMessage(data);
    return new Promise(resolve =>
      worker.addEventListener('message', resolve, { once: true })
    );
  }
}
