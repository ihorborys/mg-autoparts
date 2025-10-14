// import { createSlice } from "@reduxjs/toolkit";
//
// // const initialState = {
// //   baseFilterEndpoint: '"https://66b1f8e71ca8ad33d4f5f63e.mockapi.io/',
// //   form: "",
// //   equipment: [],
// // };
//
// const initialState = {
//   location: "", // приклад: "Kyiv"
//   vehicleType: "",
//   equipment: [],
// };
//
// const filtersSlice = createSlice({
//   name: "filters",
//   initialState,
//   reducers: {
//     setLocation(state, action) {
//       state.location = action.payload;
//     },
//     setForm(state, action) {
//       state.form = action.payload;
//     },
//     toggleEquipment(state, action) {
//       const key = action.payload;
//       if (state.equipment.includes(key)) {
//         state.equipment = state.equipment.filter((item) => item !== key);
//       } else {
//         state.equipment.push(key);
//       }
//     },
//     resetFilters() {
//       return initialState;
//     },
//   },
// });
//
// export const { setLocation, setForm, toggleEquipment, resetFilters } =
//   filtersSlice.actions;
//
// export const selectFilters = (state) => state.filters;
// export const selectEquipment = (state) => state.filters.equipment;
//
// export default filtersSlice.reducer;
// export const filtersReducer = filtersSlice.reducer;

import {createSlice} from "@reduxjs/toolkit";

const initialState = {
    location: "", // select
    equipment: [], // toggle-кнопки (як checkbox-и)
    vehicleType: "", // radio
};

const filtersSlice = createSlice({
    name: "filters",
    initialState,
    reducers: {
        // --- Локація: select
        setLocation: (state, action) => {
            state.location = action.payload;
        },

        // --- Обладнання: toggle кнопки
        setVehicleType: (state, action) => {
            state.vehicleType = action.payload;
        },

        // --- Обладнання: toggle кнопки
        toggleEquipment: (state, action) => {
            const item = action.payload;
            if (state.equipment.includes(item)) {
                state.equipment = state.equipment.filter((e) => e !== item);
            } else {
                state.equipment.push(item);
            }
        },

        // --- Очистити всі фільтри
        resetFilters: (state) => {
            state.location = "";
            state.vehicleType = "";
            state.equipment = [];
        },
    },
});


export const selectFilters = (state) => state.filters;
export const selectLocation = (state) => state.filters.location;
export const selectEquipment = (state) => state.filters.equipment;
export const selectVehicleType = (state) => state.filters.vehicleType;


export const {setLocation, setVehicleType, toggleEquipment, resetFilters} =
    filtersSlice.actions;

export const filtersReducer = filtersSlice.reducer;

// export default filtersSlice.reducer;
