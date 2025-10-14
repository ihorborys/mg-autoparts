import styles from "./Equipment.module.css";
import { EQUIPMENTS_FILTER } from "../../../utils/constants.js";
import { selectEquipment, toggleEquipment } from "../../redux/filtersSlice.js";
import { useDispatch, useSelector } from "react-redux";

const Equipment = () => {
  const selectedEquipment = useSelector(selectEquipment);
  const dispatch = useDispatch();

  const handleClickEquipments = (name) => {
    dispatch(toggleEquipment(name));
  };
  console.log(selectedEquipment);

  return (
    <div className={styles.catalogFiltersEquipment}>
      <h3 className={styles.titleEquipment}>Vehicle equipment</h3>
      <ul className={styles.equipmentList}>
        {EQUIPMENTS_FILTER.map((equipment) => (
          <li
            key={equipment.id}
            onClick={() => handleClickEquipments(equipment.name)}
            className={`${styles.equipmentItem} ${
              selectedEquipment.includes(equipment.name) ? styles.active : ""
            }`}
          >
            <input
              type="checkbox"
              className={styles.equipmentCheckbox}
              checked={selectedEquipment.includes(equipment.name)}
              readOnly
            />
            <svg className={styles.equipmentIcon} width={32} height={32}>
              <use href={equipment.icon}></use>
            </svg>
            <span className={styles.equipmentName}>{equipment.realName}</span>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default Equipment;
