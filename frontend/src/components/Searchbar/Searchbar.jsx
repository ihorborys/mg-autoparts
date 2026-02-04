import { useState } from "react";
import { useDispatch } from "react-redux";
import { fetchProductsByQuery } from "../../redux/productsOps";
import styles from "./Searchbar.module.css";


const Searchbar = () => {
  const [query, setQuery] = useState("");
  const dispatch = useDispatch();

  const handleSubmit = (e) => {
    e.preventDefault();
    if (query.trim() === "") return;

    // Відправляємо екшн в Redux, щоб запустити пошук на бекенді
    dispatch(fetchProductsByQuery(query));
  };

  return (
    <form className={styles.container} onSubmit={handleSubmit}>
      <input
        type="text"
        className={styles.input}
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Введіть артикул або бренд (наприклад: 602000700 або Luk)"
      />
      <button type="submit" className={styles.button}>
        Пошук
      </button>
    </form>
  );
};

export default Searchbar;