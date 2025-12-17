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
          <p style={{margin: '5px 300px', color: 'rgba(0, 0, 0, 1)'}}>
            ID Постачальника: <strong style={{color: 'rgba(0, 0, 0, 1)'}}>{product.supplier_id}</strong>
          </p>

          <h3 style={{margin: "0 0 10px 0", color: 'rgba(0, 0, 0, 1)'}}>
            {product.brand} -
            {/* color: '#cccccc' - світло-сірий для артикулу, щоб трохи відрізнявся */}
            <span style={{color: 'rgba(0, 0, 0, 1)', marginLeft: '5px'}}>{product.code}</span>
          </h3>

          {/* Назва товару */}
          {/* Додано color: '#e0e0e0' - дуже світлий сірий для лейблу "Назва:" */}
          <p style={{margin: '5px 0', color: 'rgba(0, 0, 0, 1)'}}>
            Назва: <strong style={{color: 'rgba(0, 0, 0, 1)'}}>{product.name}</strong>
          </p>


          {/* Блок з залишком і ціною */}
          <div style={{display: 'flex', justifyContent: 'space-between', marginTop: '15px', alignItems: 'center'}}>
            {/* Додано color: '#e0e0e0' для тексту залишку */}
            <span style={{color: 'rgba(0, 0, 0, 1)'}}>
      Залишок: {product.stock}
  </span>


            {/* Ціна. Колір #28a745 (зелений) зазвичай добре видно і на темному, залишив його. */}
            <span style={{fontSize: '1.3em', fontWeight: 'bold', color: '#28a745'}}>
     € {product.price_eur?.toFixed(2)}
              {product.supplier_id}
   </span>
          </div>
        </li>
      ))}
    </ul>
  );
};

export default CatalogList;