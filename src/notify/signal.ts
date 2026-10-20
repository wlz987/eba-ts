export const READ = 1;
export const TIMER = 2;
export const WORK = 4;
export const DELIVERY = 8;
const MASK = 0xffffffff;

export interface AgendaLike {
  readonly signals: Map<string, WeakRef<Signal>>;
  enqueue(actor: string): void;
  unqueue(actor: string): void;
}

export class Signal {
  private flags = 0;
  private owner: WeakRef<AgendaLike> | null = null;
  private id: string | null = null;

  get isBound(): boolean {
    return this.owner !== null && this.owner.deref() !== undefined;
  }

  get handed(): boolean {
    return this.owner !== null;
  }

  boundTo(owner: AgendaLike): boolean {
    return this.owner !== null && this.owner.deref() === owner;
  }

  has(flag: number): boolean {
    return (this.flags & flag) !== 0;
  }

  notify(flag: number): void {
    this.flags |= flag;
    if (this.id === null || this.owner === null) return;
    const owner = this.owner.deref();
    if (owner !== undefined) owner.enqueue(this.id);
  }

  clear(flag: number): void {
    this.flags &= ~flag & MASK;
    if (this.flags !== 0 || this.id === null || this.owner === null) return;
    const owner = this.owner.deref();
    if (owner !== undefined) owner.unqueue(this.id);
  }

  set(flag: number, ready: boolean): void {
    if (ready) this.notify(flag);
    else this.clear(flag);
  }

  bind(owner: AgendaLike, actor: string): void {
    this.owner = new WeakRef(owner);
    this.id = actor;
    owner.signals.set(actor, new WeakRef(this));
    if (this.flags) owner.enqueue(actor);
  }

  detach(): void {
    this.owner = null;
    this.id = null;
  }
}
