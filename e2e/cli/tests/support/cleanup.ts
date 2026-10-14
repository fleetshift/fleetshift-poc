type CleanupAction = () => Promise<void> | void;

/** Per-test LIFO cleanup. Every action runs; failures are aggregated. */
export class CleanupStack {
  readonly #actions: CleanupAction[] = [];

  add(action: CleanupAction): void {
    this.#actions.push(action);
  }

  async run(): Promise<void> {
    const errors: unknown[] = [];
    for (const action of this.#actions.reverse()) {
      try {
        await action();
      } catch (error) {
        errors.push(error);
      }
    }
    this.#actions.length = 0;
    if (errors.length > 0) {
      throw new AggregateError(errors, "cleanup failed");
    }
  }
}
