import type { RunEvent } from "$lib/flow/types";

type Listener = (ev: RunEvent) => void;

export class RunEventBus {
  private listeners = new Set<Listener>();

  emit(ev: RunEvent) {
    for (const fn of this.listeners) fn(ev);
  }

  subscribe(fn: Listener) {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }
}

// one bus per runId
const buses = new Map<string, RunEventBus>();

export function getBus(runId: string) {
  let bus = buses.get(runId);
  if (!bus) {
    bus = new RunEventBus();
    buses.set(runId, bus);
  }
  return bus;
}
