import {Outlet} from "react-router-dom";
import Navigation from "../../components/Navigation/MainNav/Navigation.jsx";
import Footer from "../../components/Footer/ Footer.jsx";
import Header from "../../components/Header/Header.jsx";

const CatalogDetailsPageLayout = () => {
    return (
        <div>
            <Header/>
            <Navigation/>
            <Outlet/>
            <Footer/>
        </div>
    );
};

export default CatalogDetailsPageLayout;
