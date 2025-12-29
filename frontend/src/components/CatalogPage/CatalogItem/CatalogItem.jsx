import styles from './CatalogItem.module.css';


const CatalogItem = ({product}) => {
  console.log('Мій продукт:', product);

  // Визначаємо, який клас додати
  const stockColorClass = product.stock === 0
    ? styles.outOfStock    // Випадок 1: Немає в наявності (0)
    : product.stock < 5
      ? styles.lowStock    // Випадок 2: Закінчується (1-4 шт.)
      : styles.goodStock;  // Випадок 3: Багато (5+)

  return (
    <li className={styles.wrapper}>
      <div className={styles.container}>
        <section>
          <h4 className={styles.brand}>{product.brand}</h4>
          <p className={styles.unicode}>{product.unicode}</p>
          <img src="../../../../public/img/catalog/luk_602000700.jpg" alt="../../../../public/img/catalog/no_item.png"
               width="150px"/>
        </section>

        <section className={styles.section}>
          <p className={styles.name}>{product.name}</p>
        </section>

        <section className={styles.section}>
          {/* Об'єднуємо базовий клас і динамічний колір */}
          <p className={`${styles.stock} ${stockColorClass}`}>
            {product.stock}
          </p>

        </section>
        <section className={styles.section} npm>
          <p className={styles.price}>
            {product.price_eur}
          </p>
        </section>


        {/*<p style={{margin: '5px 300px', color: 'rgba(0, 0, 0, 1)'}}>*/}
        {/*  ID Постачальника: <strong style={{color: 'rgba(0, 0, 0, 1)'}}>{product.supplier_id}</strong>*/}
        {/*</p>*/}


        {/*<p style={{margin: '5px 0', color: 'rgba(0, 0, 0, 1)'}}>*/}
        {/*  Назва: <strong style={{color: 'rgba(0, 0, 0, 1)'}}>{product.name}</strong>*/}
        {/*</p>*/}

        {/*<div style={{display: 'flex', justifyContent: 'space-between', marginTop: '15px', alignItems: 'center'}}>*/}
        {/*<span style={{color: 'rgba(0, 0, 0, 1)'}}>*/}
        {/*  Залишок: {product.stock}*/}
        {/*</span>*/}

        {/*  <span style={{fontSize: '1.3em', fontWeight: 'bold', color: '#28a745'}}>*/}
        {/*  € {Number(product.price_eur).toFixed(2)}*/}
        {/*</span>*/}
        {/*</div>*/}
      </div>

    </li>
  );
};

export default CatalogItem;