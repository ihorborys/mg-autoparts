import styles from "./HomePage.module.css";
import Button from "../../components/Button/Button.jsx";
import { Link } from "react-router-dom";

const HomePage = () => {
  return (
    <section className={styles.sectionHero}>
      <div className={styles.heroContainer}>
        <div className={styles.heroContent}>
          <h1 className={styles.heroTitle}>Campers of your dreams</h1>
          <h3 className={styles.heroSubTitle}>
            You can find everything you want in our catalog
          </h3>
          <Link to="/catalog">
            <Button>View Now</Button>
          </Link>
        </div>
      </div>
    </section>
  );
};

export default HomePage;
