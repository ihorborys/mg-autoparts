import styles from "./Address.module.css";

const Address = () => {
    return (
        <address className={styles.address}>
            <ul className={styles.addressList}>
                <li className={styles.addressItem}>
                    <svg className={styles.addressItemIcon} width="16" height="16">
                        <use href="/icons.svg#icon-whatsapp"></use>
                    </svg>
                    <a className={styles.addressLink} href="tel:+380970134331">
                        +38 (097) 013-43-31
                    </a>
                </li>

                <li className={styles.addressItem}>
                    <svg className={styles.addressItemIcon} width="16" height="16">
                        <use href="/icons.svg#icon-mail"></use>
                    </svg>
                    <a className={styles.addressLink} href="mailto:contact@maxgear.com.ua">
                        contact@maxgear.com.ua
                    </a>
                </li>
            </ul>
        </address>
    );
};

export default Address;
