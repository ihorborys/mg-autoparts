import axios from "axios";
import { createAsyncThunk } from "@reduxjs/toolkit";

axios.defaults.baseURL = "https://66b1f8e71ca8ad33d4f5f63e.mockapi.io/";

export const fetchCampers = createAsyncThunk(
  "campers/fetch-all",
  async (params, thunkAPI) => {
    try {
      const {
        limit = 4,
        page = 1,
        filters = { location: "", vehicleType: "", equipment: [] },
      } = params;

      const queryParams = new URLSearchParams();

      queryParams.set("limit", limit);
      queryParams.set("page", page);

      // Додаємо фільтри, якщо є
      if (filters.location) queryParams.set("location", filters.location);
      if (filters.vehicleType) queryParams.set("form", filters.vehicleType);
      // Обладнання — кожен ключ як параметр
      if (filters.equipment && filters.equipment.length) {
        filters.equipment.forEach((eq) => queryParams.set(eq, "true"));
      }

      const { data } = await axios.get(`campers?${queryParams.toString()}`);
      return data;
    } catch (error) {
      return thunkAPI.rejectWithValue(error.message);
    }
  },
);

export const getSelectedCamper = createAsyncThunk(
  "campers/get-selected-camper",
  async (id, thunkAPI) => {
    try {
      const { data } = await axios(`campers/${id}`);
      return data;
    } catch (error) {
      return thunkAPI.rejectWithValue(error.message);
    }
  },
);
