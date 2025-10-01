import styles from "./NotFoundPage.module.css";
import {Link} from "react-router-dom";

const NotFoundPage = () => {
    return (
        <div className={styles.container}>
            <div className={styles.message}>Вибачте, сторінку не знайдено... (((</div>
            <Link to={"/"} className={styles.link}>
                <div className={styles.link}>На головну</div>
            </Link>
        </div>
    );
};

export default NotFoundPage;
