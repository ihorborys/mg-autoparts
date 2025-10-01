import {NavLink} from "react-router-dom";
import styles from "./Navigation.module.css";

const navClasses = ({isActive}) => {
    return isActive ? styles.active : "";
};

const Navigation = () => {
    return (
        <section className={styles.sectionNavigation}>
            <nav className={styles.navigationContainer}>
                <ul className={styles.navigationList}>
                    <li className={styles.navigationLogo}>
                        <NavLink to="/" className={styles.navigationLogoLink}>
                            <img src="/logo.webp" alt="Logo" className={styles.navigationIcon}/>
                        </NavLink>
                    </li>
                    <li className={styles.navigationGroup}>
                        <ul className={styles.navigationItemContainer}>
                            <li className={styles.navigationItem}>
                                <NavLink to="/" className={navClasses}>
                                    Головна
                                </NavLink>
                            </li>
                            <li className={styles.navigationItem}>
                                <NavLink to="/catalog" className={navClasses}>
                                    Каталог
                                </NavLink>
                            </li>
                        </ul>
                    </li>
                </ul>
            </nav>
        </section>
    );
};

export default Navigation;
