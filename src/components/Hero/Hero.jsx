import styles from './Hero.module.css';
import Container from "../../layouts/Container/Container.jsx";
import {Link} from "react-router-dom";
import Button from "../Button/Button.jsx";

const Hero = () => {
    return (
        <section className={styles.wrapper}>
            <Container>
                <div className={styles.container}>
                    <div className={styles.content}>
                        <h1 className={styles.title}>Автозапчастини, які Вам потрібні</h1>
                        <h3 className={styles.subTitle}>
                            Ви можете знайти все, що потрібно у нашому каталозі
                        </h3>
                        <Link to="/catalog">
                            <Button>До каталогу</Button>
                        </Link>

                    </div>
                </div>
            </Container>
        </section>
    );
};

export default Hero;


