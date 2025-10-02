import styles from './Footer.module.css';
import Container from "../../layouts/Container/Container.jsx";
import Address from "../Address/Address.jsx";

const Footer = () => {
    const currentYear = new Date().getFullYear();

    return (
        <footer className={styles.wrapper}>
            <Container>
                <div className={styles.container}>
                    <Address/>
                    
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
        </footer>
    );
};

export default Footer;
