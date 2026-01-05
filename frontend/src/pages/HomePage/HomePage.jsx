import Hero from "../../components/Hero/Hero.jsx";
import Slider from "../../components/Slider/Slider.jsx";
import styles from "../HomePage/HomePage.module.css";

const HomePage = () => {
  return (
    <div className={styles.containerHomePage}>
      <Hero></Hero>
      <Slider/>
    </div>
  );
};

export default HomePage;
