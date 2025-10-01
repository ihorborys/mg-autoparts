import styles from './Footer.module.css';
import Container from "../../layouts/Container/Container.jsx";

const Footer = () => {
    const currentYear = new Date().getFullYear();

    return (
        <footer>
            <div className={styles.wrapper}>
                <Container>
                    <div className={styles.container}>
                        <address className={styles.address}>
                            <ul className={styles.addressList}>
                                <li className={styles.addressItem}>
                                    <svg className={styles.icon} height="16" width="16">
                                        <use href="/icons.svg#icon-whatsapp"/>
                                    </svg>
                                    <a className={styles.link} href="tel:+380970134331">
                                        +38 (097) 013-43-31
                                    </a>
                                </li>
                                <li className={styles.addressItem}>
                                    <svg className={styles.icon} height="16" width="16">
                                        <use href="/icons.svg#icon-mail"/>
                                    </svg>
                                    <a className={styles.link} href="mailto:contact@maxgear.com.ua">
                                        contact@maxgear.com.ua
                                    </a>
                                </li>
                            </ul>
                        </address>

                        <nav className={styles.nav}>
                            <a className={styles.logo} href="/">
                                <img
                                    src={"/logo.webp"}
                                    alt="Логотип Maxgear"
                                    width="176"
                                    className={styles.logoImg}
                                />
                            </a>
                        </nav>

                        <small className={styles.copyright}>
                            © {currentYear} &nbsp;|&nbsp;
                            <a
                                className={styles.copyrightLink}
                                href="https://www.linkedin.com/in/ihor-borys/"
                                target="_blank"
                                rel="noopener noreferrer"
                            >
                                Ihor Borys
                            </a>
                            &nbsp;|&nbsp; Усі права захищено
                        </small>
                    </div>
                </Container>
            </div>
        </footer>
    );
};

export default Footer;
