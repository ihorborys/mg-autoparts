import { fetchCampers, getSelectedCamper } from "./campersOps.js";
import { createSlice } from "@reduxjs/toolkit";

const initialState = {
  items: [],
  item: null,
  currentPage: 1,
  limit: 4,
  totalPages: null,
  loading: false,
  error: null,
};

const campersSlice = createSlice({
  name: "campers",
  initialState,
  reducers: {
    setPage(state, action) {
      state.currentPage = action.payload;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchCampers.pending, (state) => {
        state.loading = true;
      })
      .addCase(fetchCampers.fulfilled, (state, { payload }) => {
        state.error = null;

        const totalItems = payload.total;

        if (state.currentPage === 1) {
          state.items = payload.items; // при першому запиті — замінюємо
        } else {
          state.items = [...state.items, ...payload.items]; // додаємо нові
        }

        state.totalPages = Math.ceil(totalItems / state.limit);
        state.loading = false;
      })
      .addCase(fetchCampers.rejected, (state) => {
        // state.error = payload;
        state.items = [];
        state.error = `Sorry, can't find anything...`;
        state.loading = false;
      })
      .addCase(getSelectedCamper.pending, (state) => {
        state.loading = true;
      })
      .addCase(getSelectedCamper.fulfilled, (state, { payload }) => {
        state.item = payload;
        state.loading = false;
      })
      .addCase(getSelectedCamper.rejected, (state, { payload }) => {
        state.error = payload;
        state.loading = false;
      });
  },
});

export const { setPage } = campersSlice.actions;

export const selectCampers = (state) => state.campers.items;
export const selectCamper = (state) => state.campers.item;

export const selectCurrentPage = (state) => state.campers.currentPage;
export const selectLimit = (state) => state.campers.limit;

export const selectLoading = (state) => state.campers.loading;
export const selectError = (state) => state.campers.error;

export const campersReducer = campersSlice.reducer;
