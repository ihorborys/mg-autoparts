import styles from "./Header.module.css";
import Container from "../../layouts/Container/Container.jsx";
import Address from "../Address/Address.jsx";

const Header = () => {
    return (
        <header className={styles.wrapper}>
            <Container>
                <div className={styles.container}>
                    
                    <Address/>

                    {/*<nav className={styles.nav}>*/}
                    {/*    <a className={styles.logo} href="./index.html">*/}
                    {/*        <img*/}
                    {/*            alt="Логотип Maxgear"*/}
                    {/*            className={styles.navImg}*/}
                    {/*            src="/img/header/maxgear-logo-white-small-crop.webp"*/}
                    {/*            width="176"*/}
                    {/*        />*/}
                    {/*    </a>*/}

                    {/*    <ul className={styles.headerMenu}>*/}
                    {/*        <li className={styles.headerItem}>*/}
                    {/*            <a className={`${styles.headerLink} ${styles.active}`} href="./index.html">*/}
                    {/*                Постачальники*/}
                    {/*            </a>*/}
                    {/*        </li>*/}
                    {/*        <li className={styles.headerItem}>*/}
                    {/*            <a className={styles.headerLink} href="#">*/}
                    {/*                Прайси*/}
                    {/*            </a>*/}
                    {/*        </li>*/}
                    {/*        <li className={styles.headerItem}>*/}
                    {/*            <a className={styles.headerLink} href="#">*/}
                    {/*                Співпраця*/}
                    {/*            </a>*/}
                    {/*        </li>*/}
                    {/*    </ul>*/}

                    {/*    Тут буде компонент бургер-меню*/}
                    {/*    <BurgerMenu/>*/}
                    {/*</nav>*/}
                </div>
            </Container>
        </header>
    );
}

export default Header;
