import {RingLoader} from "react-spinners";
import styles from "./Loader.module.css";

const Loader = () => {
    const redColor = getComputedStyle(document.documentElement)
        .getPropertyValue("--red")
        .trim();

    return (
        <div className={styles.loaderWrapper}>
            <RingLoader size={60} color={redColor}/>
        </div>
    );
};
export default Loader;
