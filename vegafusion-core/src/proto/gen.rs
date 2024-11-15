// Create separate modules for each proto component
pub mod errors {
    include!(concat!(env!("OUT_DIR"), "/errors.rs"));
}

pub mod expression {
    include!(concat!(env!("OUT_DIR"), "/expression.rs"));
}

pub mod pretransform {
    include!(concat!(env!("OUT_DIR"), "/pretransform.rs"));
}

pub mod services {
    include!(concat!(env!("OUT_DIR"), "/services.rs"));
}

pub mod tasks {
    include!(concat!(env!("OUT_DIR"), "/tasks.rs"));
}

pub mod transforms {
    include!(concat!(env!("OUT_DIR"), "/transforms.rs"));
}
