import styles from "./Filters.module.css";
import Button from "../Button/Button.jsx";
import VehicleType from "./VehicleType/VehicleType.jsx";
import LocationFilter from "./Location/Location.jsx";
import Equipment from "./Equipment/Equipment.jsx";
import { fetchCampers } from "../redux/campersOps.js";
import { useDispatch, useSelector } from "react-redux";
import { selectFilters } from "../redux/filtersSlice.js";
import { selectCurrentPage, selectLimit } from "../redux/campersSlice.js";

const Filters = () => {
  const currentPage = useSelector(selectCurrentPage);
  const limit = useSelector(selectLimit);
  const dispatch = useDispatch();
  const filters = useSelector(selectFilters);

  const fetchFilteredCampers = () => {
    dispatch(fetchCampers({ page: currentPage, limit, filters }));
  };
  return (
    <div className={styles.catalogFiltersContainer}>
      <LocationFilter />
      <h3 className={styles.titleFiltersEquipment}>Filters</h3>
      <Equipment />
      <VehicleType />
      <Button handleOnClick={fetchFilteredCampers} className={styles.searchBtn}>
        Search
      </Button>
    </div>
  );
};
export default Filters;
