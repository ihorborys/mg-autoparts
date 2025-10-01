import {NavLink} from "react-router-dom";
import styles from "./Navigation.module.css";
import Container from "../../../layouts/Container/Container.jsx";

const navClasses = ({isActive}) => {
    return isActive ? styles.active : "";
};

const Navigation = () => {
    return (
        <section className={styles.wrapper}>
            <Container>
                <nav className={styles.container}>
                    <ul className={styles.list}>
                        <li className={styles.logo}>
                            <NavLink to="/" className={styles.logoLink}>
                                <img src="/logo.webp" alt="Logo" className={styles.icon}/>
                            </NavLink>
                        </li>
                        <li className={styles.group}>
                            <ul className={styles.itemContainer}>
                                <li className={styles.item}>
                                    <NavLink to="/" className={navClasses}>
                                        Головна
                                    </NavLink>
                                </li>
                                <li className={styles.item}>
                                    <NavLink to="/catalog" className={navClasses}>
                                        Каталог
                                    </NavLink>
                                </li>
                            </ul>
                        </li>
                    </ul>
                </nav>
            </Container>
        </section>
    );
};

export default Navigation;
