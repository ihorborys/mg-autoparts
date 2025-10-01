import styles from "./CatalogPage.module.css";
import CatalogList from "../../components/CatalogList/CatalogList.jsx";
import Filters from "../../components/Filters/Filters.jsx";
import { useDispatch, useSelector } from "react-redux";
import { useEffect } from "react";
import { fetchCampers } from "../../components/redux/campersOps.js";
import {
  selectCurrentPage,
  selectLimit,
} from "../../components/redux/campersSlice.js";

const CatalogPage = () => {
  const currentPage = useSelector(selectCurrentPage);
  const limit = useSelector(selectLimit);

  const dispatch = useDispatch();
  useEffect(() => {
    dispatch(fetchCampers({ page: currentPage, limit }));
  }, [dispatch, currentPage, limit]);

  return (
    <div className={styles.catalogContainer}>
      <div className={styles.catalogPageWrapper}>
        <section className={styles.catalogFilters}>
          <Filters />
        </section>
        <section className={styles.catalogList}>
          <CatalogList />
        </section>
      </div>
    </div>
  );
};

export default CatalogPage;
