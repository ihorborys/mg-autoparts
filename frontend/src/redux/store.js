import { configureStore } from "@reduxjs/toolkit";

// Імпортуємо наш НОВИЙ редьюсер для продуктів
import { productsReducer } from "./productsSlice";


const rootReducer = {
  // Підключаємо наш productsReducer під ключем 'products'.
  // Тепер весь стан, пов'язаний з товарами, буде доступний як state.products
  products: productsReducer,
};

export const store = configureStore({
  reducer: rootReducer,
});