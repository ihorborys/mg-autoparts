import styles from "./Button.module.css";

const Button = ({ handleOnClick, children }) => {
  return (
    <button className={styles.container} onClick={handleOnClick}>
      {children}
    </button>
  );
};
export default Button;
