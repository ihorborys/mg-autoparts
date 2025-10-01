import { NavLink } from "react-router-dom";
import styles from "./CatalogDetailsPageNav.module.css";

const navClasses = ({ isActive }) => {
  return isActive ? styles.active : "";
};

const CatalogDetailsPageNav = () => {
  return (
    <ul className={styles.navigationList}>
      <li className={styles.navigationItem}>
        <NavLink to="features" className={navClasses}>
          Features
        </NavLink>
      </li>
      <li className={styles.navigationItem}>
        <NavLink to="reviews" className={navClasses}>
          Reviews
        </NavLink>
      </li>
    </ul>
  );
};

export default CatalogDetailsPageNav;
