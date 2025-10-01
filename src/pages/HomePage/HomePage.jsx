import styles from "./HomePage.module.css";
import Button from "../../components/Button/Button.jsx";
import {Link} from "react-router-dom";

const HomePage = () => {
    return (
        <section className={styles.sectionHero}>
            <div className={styles.heroContainer}>
                <div className={styles.heroContent}>
                    <h1 className={styles.heroTitle}>Автозапчастини, які Вам потрібні</h1>
                    <h3 className={styles.heroSubTitle}>
                        Ви можете знайти все, що потрібно у нашому каталозі
                    </h3>
                    <Link to="/catalog">
                        <Button>До каталогу</Button>
                    </Link>
                </div>
            </div>
        </section>
    );
};

export default HomePage;
