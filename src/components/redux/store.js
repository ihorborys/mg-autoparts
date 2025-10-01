import { configureStore } from "@reduxjs/toolkit";
import { campersReducer } from "./campersSlice.js";
import { filtersReducer } from "./filtersSlice.js";
import { favoritesReducer } from "./favoritesSlice.js";

const rootReducer = {
  campers: campersReducer,
  filters: filtersReducer,
  favorites: favoritesReducer,
};

export const store = configureStore({
  reducer: rootReducer,
});
