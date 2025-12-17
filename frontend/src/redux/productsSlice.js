import { createSlice } from "@reduxjs/toolkit";
import { fetchProductsByQuery } from "./productsOps";

const initialState = {
  items: [],       // Масив знайдених товарів
  isLoading: false, // Індикатор завантаження
  error: null,     // Текст помилки, якщо вона є
};

const productsSlice = createSlice({
  name: "products",
  initialState,
  // Тут ми обробляємо результати асинхронного запиту fetchProductsByQuery
  extraReducers: (builder) => {
    builder
      .addCase(fetchProductsByQuery.pending, (state) => {
        state.isLoading = true;
        state.error = null;
      })
      .addCase(fetchProductsByQuery.fulfilled, (state, action) => {
        state.isLoading = false;
        state.items = action.payload; // Записуємо отримані товари у стейт
      })
      .addCase(fetchProductsByQuery.rejected, (state, action) => {
        state.isLoading = false;
        state.error = action.payload;
      });
  },
});

// Експортуємо редьюсер
export const productsReducer = productsSlice.reducer;