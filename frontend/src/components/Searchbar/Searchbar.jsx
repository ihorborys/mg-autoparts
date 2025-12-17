// src/components/Searchbar/Searchbar.jsx
import { useState } from "react";
import { useDispatch } from "react-redux";
import { fetchProductsByQuery } from "../../redux/productsOps";

const styles = {
  container: {
    display: 'flex',
    gap: '10px',
    marginBottom: '20px',
    padding: '20px',
    border: '1px solid #ddd',
    borderRadius: '8px',
    backgroundColor: '#f9f9f9'
  },
  input: {
    flexGrow: 1,
    padding: '12px',
    fontSize: '16px',
    border: '1px solid #ccc',
    borderRadius: '4px'
  },
  button: {
    padding: '12px 24px',
    fontSize: '16px',
    backgroundColor: '#007bff',
    color: 'white',
    border: 'none',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 'bold'
  }
};

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
    <form style={styles.container} onSubmit={handleSubmit}>
      <input
        type="text"
        style={styles.input}
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Введіть артикул або бренд (напр. febest)..."
      />
      <button type="submit" style={styles.button}>
        Пошук
      </button>
    </form>
  );
};

export default Searchbar;