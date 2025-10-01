import styles from "./header.module.css";

export default function Header() {
    return (
        <header className={styles.header}>
            <div className={styles.wrapper}>
                <div className={styles.container}>
                    <address className={styles.address}>
                        <ul className={styles.addressList}>
                            <li className={styles.addressItem}>
                                <svg className={styles.addressItemIcon} height="16" width="16">
                                    <use href="/img/icons.svg#icon-whatsapp"></use>
                                </svg>
                                <a className={styles.addressLink} href="tel:+380970134331">
                                    +38 (097) 013-43-31
                                </a>
                            </li>
                            <li className={styles.addressItem}>
                                <svg className={styles.addressItemIcon} height="16" width="16">
                                    <use href="/img/icons.svg#icon-mail"></use>
                                </svg>
                                <a className={styles.addressLink} href="mailto:contact@maxgear.com.ua">
                                    contact@maxgear.com.ua
                                </a>
                            </li>
                        </ul>
                    </address>

                    <nav className={styles.nav}>
                        <a className={styles.logo} href="./index.html">
                            <img
                                alt="Логотип Maxgear"
                                className={styles.navImg}
                                src="/img/header/maxgear-logo-white-small-crop.webp"
                                width="176"
                            />
                        </a>

                        {/*<ul className={styles.headerMenu}>*/}
                        {/*    <li className={styles.headerItem}>*/}
                        {/*        <a className={`${styles.headerLink} ${styles.active}`} href="./index.html">*/}
                        {/*            Постачальники*/}
                        {/*        </a>*/}
                        {/*    </li>*/}
                        {/*    <li className={styles.headerItem}>*/}
                        {/*        <a className={styles.headerLink} href="#">*/}
                        {/*            Прайси*/}
                        {/*        </a>*/}
                        {/*    </li>*/}
                        {/*    <li className={styles.headerItem}>*/}
                        {/*        <a className={styles.headerLink} href="#">*/}
                        {/*            Співпраця*/}
                        {/*        </a>*/}
                        {/*    </li>*/}
                        {/*</ul>*/}

                        {/* Тут буде компонент бургер-меню */}
                        {/* <BurgerMenu /> */}
                    </nav>
                </div>
            </div>
        </header>
    );
}
