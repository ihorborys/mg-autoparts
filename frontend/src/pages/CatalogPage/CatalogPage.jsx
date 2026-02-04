import styles from "./CatalogPage.module.css";
import Searchbar from "../../components/Searchbar/Searchbar";
import CatalogList from "../../components/CatalogPage/CatalogList/CatalogList";
import Container from "../../layouts/Container/Container.jsx";

const CatalogPage = () => {
  return (
    <Container>
      <div className={styles.containerCatalogPage}>
        <section>FILTERS</section>
        <section>
          <h2 className={styles.title}>Каталог Автозапчастин</h2>
          <Searchbar/>
          <CatalogList/>
        </section>
      </div>
    </Container>
  );
};

export default CatalogPage;