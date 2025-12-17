// src/components/CatalogList/CatalogList.jsx
import { useSelector } from "react-redux";
import Loader from "../Loader/Loader";

const CatalogList = () => {
  // Дістаємо дані (товари, статус завантаження, помилки) з Redux store
  const {items, isLoading, error} = useSelector((state) => state.products);

  if (isLoading) return <Loader/>;
  if (error) return <p style={{color: 'red', textAlign: 'center'}}>Помилка: {error}</p>;

  // Якщо запиту ще не було або нічого не знайдено
  if (items.length === 0) {
    return <p style={{textAlign: 'center', marginTop: '20px', color: '#666'}}>
      Введіть запит у пошук (наприклад, 'febest'), щоб побачити товари.
    </p>;
  }

  return (
    <ul style={{listStyle: "none", padding: 0}}>
      {items.map((product) => (
        // Використовуємо комбінацію коду і постачальника як унікальний ключ
        <li
          key={`${product.code}-${product.supplier_id}`}
          style={{
            border: "1px solid #eee",
            borderRadius: '8px',
            margin: "10px 0",
            padding: "15px",
            backgroundColor: '#fff',
            boxShadow: '0 2px 4px rgba(0,0,0,0.05)'
          }}
        >
          <h3 style={{margin: "0 0 10px 0", color: '#333'}}>
            {product.brand} - <span style={{color: '#555'}}>{product.code}</span>
          </h3>
          <p style={{margin: '5px 0'}}>Назва: <strong>{product.name}</strong></p>
          <div style={{display: 'flex', justifyContent: 'space-between', marginTop: '15px'}}>
            <span>Залишок: {product.stock}</span>
            {/* Відображаємо ціну з бекенду */}
            <span style={{fontSize: '1.2em', fontWeight: 'bold', color: '#28a745'}}>
               € {product.price_eur?.toFixed(2)}
             </span>
          </div>
        </li>
      ))}
    </ul>
  );
};

export default CatalogList;