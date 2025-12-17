import { createAsyncThunk } from "@reduxjs/toolkit";
import axios from "axios";

// Вказуємо адресу вашого локального бекенду
axios.defaults.baseURL = "http://localhost:8000";

// Асинхронний thunk для пошуку товарів
export const fetchProductsByQuery = createAsyncThunk(
  "products/fetchByQuery",
  async (query, thunkAPI) => {
    try {
      // Робимо GET запит: /api/search?q=...&limit=20
      const response = await axios.get("/api/search", {
        params: {
          q: query,
          limit: 20, // Можна змінити ліміт
        },
      });
      // Повертаємо дані (масив товарів), які прийшли з бекенду
      return response.data;
    } catch (error) {
      // Якщо сталася помилка, повертаємо текст помилки
      return thunkAPI.rejectWithValue(error.message);
    }
  }
);