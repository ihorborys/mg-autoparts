import { useSelector } from "react-redux";
import Loader from "../../Loader/Loader.jsx";
import CatalogItem from "../CatalogItem/CatalogItem"; // Імпортуємо наш новий компонент


const CatalogList = () => {
  const {items, isLoading, error} = useSelector((state) => state.products);

  if (isLoading) return <Loader/>;
  if (error) return <p style={{color: 'red', textAlign: 'center'}}>Помилка: {error}</p>;

  if (items.length === 0) {
    return (
      <p style={{textAlign: 'center', marginTop: '20px', color: '#666'}}>
        {/* Порожній стан */}
      </p>
    );
  }

  return (
    <ul style={{listStyle: "none", padding: 0}}>
      {items.map((product) => (
        // Ключ (key) ЗАВЖДИ має бути тут, у списку, а не всередині компонента
        <CatalogItem
          key={`${product.code}-${product.supplier_id}`}
          product={product}
        />
      ))}
    </ul>
  );
};

export default CatalogList;