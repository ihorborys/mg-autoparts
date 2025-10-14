import styles from "./ErrorMessage.module.css";

const ErrorMessage = ({ errorMessage }) => {
  return (
    <div className={styles.errorWrapper}>
      <p className={styles.errorMessage}>{errorMessage}</p>
    </div>
  );
};

export default ErrorMessage;
