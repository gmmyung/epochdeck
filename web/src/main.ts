import { mount } from "svelte";

import App from "./App.svelte";
import "./styles.css";

const target = document.getElementById("app");

if (!target) {
  throw new Error("Runloom dashboard mount point is missing");
}

mount(App, { target });
