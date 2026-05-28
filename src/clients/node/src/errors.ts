export class DurableStepFailed extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DurableStepFailed";
  }
}

export class DurableStepInProgress extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DurableStepInProgress";
  }
}
