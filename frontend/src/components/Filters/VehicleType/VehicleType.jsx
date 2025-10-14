import styles from "./VehicleType.module.css";
import { useDispatch, useSelector } from "react-redux";
import { selectVehicleType, setVehicleType } from "../../redux/filtersSlice.js";
import { TYPES } from "../../../utils/constants.js";

const VehicleType = () => {
  const selectedType = useSelector(selectVehicleType);
  const dispatch = useDispatch();

  const handleClickTypes = (name) => {
    dispatch(setVehicleType(name));
  };
  console.log(selectedType);

  return (
    <div className={styles.catalogFiltersType}>
      <h3 className={styles.titleType}>Vehicle type</h3>
      <div className={styles.typeList}>
        {TYPES.map((type) => {
          return (
            <button
              key={type.id}
              type="button"
              className={`${styles.typeItem} ${selectedType === type.name ? styles.active : ""}`}
              onClick={() => handleClickTypes(type.name)}
            >
              <svg className={styles.typeIcon} width={32} height={32}>
                <use href={type.icon}></use>
              </svg>
              <span className={styles.typeName}>{type.realName}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
};

export default VehicleType;
