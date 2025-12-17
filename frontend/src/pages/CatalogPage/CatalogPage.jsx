// src/pages/CatalogPage/CatalogPage.jsx
import Searchbar from "../../components/Searchbar/Searchbar";
import CatalogList from "../../components/CatalogList/CatalogList";

const CatalogPage = () => {
  return (
    <div style={{maxWidth: '800px', margin: '0 auto', padding: '20px'}}>
      <h1 style={{textAlign: 'center'}}>Пошук Автозапчастин</h1>
      <Searchbar/>
      <CatalogList/>
    </div>
  );
};

export default CatalogPage;