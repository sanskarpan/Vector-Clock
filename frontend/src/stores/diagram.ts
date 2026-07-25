// Derived store: computed D3 layout from events
import { computed } from 'nanostores'
import { simulationStore } from './simulation'
import { computeLayout } from '../components/SpaceTimeDiagram/layout'

export const diagramStore = computed(simulationStore, (state) => {
  const processes = Array.from(state.processes.values())
  return computeLayout(state.events, processes, { clockType: state.config.clockType })
})
