type Item<T> = [at: number, sequence: number, itemId: T];

export const ELAPSED = 0;

export class Deadlines<T = string> {
  private items: Item<T>[] = [];
  private positions = new Map<T, number>();
  private sequence = 0;

  at(itemId: T): number | null {
    const index = this.positions.get(itemId);
    if (index === undefined) return null;
    return this.items[index]![0];
  }

  get nextMs(): number | null {
    if (this.items.length === 0) return null;
    return this.items[0]![0];
  }

  add(itemId: T, at: number): void {
    if (this.positions.has(itemId)) throw new Error("duplicate deadline");
    this.sequence += 1;
    this.positions.set(itemId, this.items.length);
    this.items.push([at, this.sequence, itemId]);
    this.up(this.items.length - 1);
  }

  remove(itemId: T): void {
    const index = this.positions.get(itemId);
    if (index === undefined) return;
    this.positions.delete(itemId);
    const last = this.items.pop()!;
    if (index === this.items.length) return;
    this.items[index] = last;
    this.positions.set(last[2], index);
    this.up(index);
    this.down(index);
  }

  peekDue(nowMs: number): T | null {
    if (this.items.length === 0 || this.items[0]![0] > nowMs) return null;
    return this.items[0]![2];
  }

  private before(left: Item<T>, right: Item<T>): boolean {
    return left[0] < right[0] || (left[0] === right[0] && left[1] < right[1]);
  }

  private swap(left: number, right: number): void {
    const a = this.items[left]!;
    const b = this.items[right]!;
    this.items[left] = b;
    this.items[right] = a;
    this.positions.set(a[2], right);
    this.positions.set(b[2], left);
  }

  private up(index: number): void {
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (!this.before(this.items[index]!, this.items[parent]!)) return;
      this.swap(index, parent);
      index = parent;
    }
  }

  private down(index: number): void {
    const length = this.items.length;
    while (2 * index + 1 < length) {
      let child = 2 * index + 1;
      if (
        child + 1 < length &&
        this.before(this.items[child + 1]!, this.items[child]!)
      ) {
        child += 1;
      }
      if (!this.before(this.items[child]!, this.items[index]!)) return;
      this.swap(index, child);
      index = child;
    }
  }
}
