import type { Component } from 'solid-js';
import Simulation from './components/Simulation'

const App: Component = () => {
  return (
    <div class="w-full h-full flex flex-col">
      <Simulation />
    </div>
  );
};

export default App;
