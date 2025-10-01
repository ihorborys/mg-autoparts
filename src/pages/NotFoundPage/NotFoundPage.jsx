import styles from "./NotFoundPage.module.css";
import { Link } from "react-router-dom";

const NotFoundPage = () => {
  return (
    <div className={styles.container}>
      <div className={styles.message}>Sorry, page does not exists!</div>
      <Link to={"/"} className={styles.link}>
        <div className={styles.link}>Go Home</div>
      </Link>
    </div>
  );
};

export default NotFoundPage;
