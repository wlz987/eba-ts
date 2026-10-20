export function positiveInt(value: number, name: string): number {
  if (typeof value === "boolean" || !Number.isInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive integer`);
  }
  return value;
}

export function nonnegativeInt(value: number, name: string): number {
  if (typeof value === "boolean" || !Number.isInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a nonnegative integer`);
  }
  return value;
}
