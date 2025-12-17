// src/App.jsx
import "./App.css";
import { Route, Routes } from "react-router-dom";
import { lazy, Suspense } from "react";
import Loader from "./components/Loader/Loader.jsx";

// Імпортуємо тільки потрібні сторінки
// Я прибрав штучну затримку (setTimeout) для HomePage, вона не потрібна в реальному проєкті
const HomePage = lazy(() => import("./pages/HomePage/HomePage.jsx"));
const CatalogPage = lazy(() => import("./pages/CatalogPage/CatalogPage.jsx"));
const NotFoundPage = lazy(() => import("./pages/NotFoundPage/NotFoundPage.jsx"));

// Імпортуємо тільки потрібні лейаути
const HomeLayout = lazy(() => import("./layouts/HomeLayout/HomeLayout.jsx"));
const CatalogLayout = lazy(() => import("./layouts/CatalogLayout/CatalogLayout.jsx"));

function App() {
  return (
    <div>
      <Suspense fallback={<Loader/>}>
        <Routes>
          {/* Головна сторінка */}
          <Route path={"/"} element={<HomeLayout/>}>
            <Route index element={<HomePage/>}></Route>
          </Route>

          {/* Сторінка каталогу запчастин */}
          <Route path={"/catalog"} element={<CatalogLayout/>}>
            {/* Зверніть увагу: тут шлях "index", а не дублювання "/catalog" */}
            <Route index element={<CatalogPage/>}></Route>
          </Route>

          {/* Сторінка 404 */}
          <Route path="*" element={<NotFoundPage/>}/>
        </Routes>
      </Suspense>
    </div>
  );
}

export default App;